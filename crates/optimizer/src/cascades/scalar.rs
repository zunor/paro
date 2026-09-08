// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable, hash-consed scalar DAG kept outside the relational Memo.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use super::ids::{ColumnId, Fingerprint, ScalarExprId, StableFingerprintBuilder};

mod literal;
pub use literal::ScalarLiteral;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    DistinctFrom,
    NotDistinctFrom,
}

impl ComparisonOp {
    fn swapped(self) -> Self {
        match self {
            Self::Less => Self::Greater,
            Self::LessOrEqual => Self::GreaterOrEqual,
            Self::Greater => Self::Less,
            Self::GreaterOrEqual => Self::LessOrEqual,
            other => other,
        }
    }

    fn can_swap(self) -> bool {
        matches!(
            self,
            Self::Equal
                | Self::NotEqual
                | Self::Less
                | Self::LessOrEqual
                | Self::Greater
                | Self::GreaterOrEqual
                | Self::DistinctFrom
                | Self::NotDistinctFrom
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarKind {
    Constant {
        value: ScalarLiteral,
    },
    Column(ColumnId),
    /// An outer lexical scope is not a column of this relational input. Keep
    /// its depth even when the bound column id happens to be the same.
    CorrelatedColumn {
        column: ColumnId,
        depth: usize,
    },
    Parameter(u32),
    Function {
        routine: Fingerprint,
    },
    Cast {
        try_cast: bool,
    },
    And,
    Or,
    Comparison(ComparisonOp),
    Case,
    Operator {
        operator: Fingerprint,
    },
    Aggregate {
        function: Fingerprint,
    },
    BoundSubquery {
        query: Fingerprint,
    },
    Window {
        function: Fingerprint,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScalarLocalProperties {
    pub volatility: Volatility,
    pub may_error: bool,
    pub has_side_effects: bool,
    pub depends_on_external_state: bool,
    pub deterministic: bool,
}

impl Default for ScalarLocalProperties {
    fn default() -> Self {
        Self {
            volatility: Volatility::Immutable,
            may_error: false,
            has_side_effects: false,
            depends_on_external_state: false,
            deterministic: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarProperties {
    pub referenced_columns: BTreeSet<ColumnId>,
    pub volatility: Volatility,
    pub may_error: bool,
    pub has_side_effects: bool,
    pub depends_on_external_state: bool,
    pub deterministic: bool,
}

impl ScalarProperties {
    /// Re-evaluating the same immutable input is weaker than commuting with
    /// a row-removing operator. A deterministic SQL error remains the same
    /// error on replay; it does not authorize evaluation on new input rows.
    pub fn can_repeat_evaluation(&self) -> bool {
        self.volatility != Volatility::Volatile
            && !self.has_side_effects
            && !self.depends_on_external_state
            && self.deterministic
    }

    pub fn is_evaluation_fence(&self) -> bool {
        self.volatility == Volatility::Volatile
            || self.may_error
            || self.has_side_effects
            || self.depends_on_external_state
            || !self.deterministic
    }

    pub fn can_reorder_and_share(&self) -> bool {
        !self.is_evaluation_fence()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarNode {
    pub kind: ScalarKind,
    pub logical_type: LogicalType,
    pub children: Box<[ScalarExprId]>,
    pub properties: ScalarProperties,
    pub fingerprint: Fingerprint,
}

#[derive(Debug, Clone)]
pub struct ScalarSpec {
    pub kind: ScalarKind,
    pub logical_type: LogicalType,
    pub children: Box<[ScalarExprId]>,
    pub local_properties: ScalarLocalProperties,
}

#[derive(Debug, Clone, Default)]
pub struct ScalarArena {
    nodes: Vec<ScalarNode>,
    by_fingerprint: BTreeMap<Fingerprint, Vec<ScalarExprId>>,
}

impl ScalarArena {
    pub fn get(&self, id: ScalarExprId) -> Option<&ScalarNode> {
        self.nodes.get(id.index())
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Roll an append-only scalar generation back without cloning the DAG.
    /// Fingerprint buckets may contain collisions, so remove ids rather than
    /// assuming one bucket entry per node.
    pub(crate) fn truncate(&mut self, len: usize) -> Result<()> {
        if len > self.nodes.len() {
            return Err(paro_error::internal(
                "scalar arena rollback exceeds the current generation",
            ));
        }
        for index in (len..self.nodes.len()).rev() {
            let id = ScalarExprId::new(index);
            let fingerprint = self.nodes[index].fingerprint;
            let bucket = self.by_fingerprint.get_mut(&fingerprint).ok_or_else(|| {
                paro_error::internal("scalar fingerprint index lost an arena node")
            })?;
            let position = bucket
                .iter()
                .position(|candidate| *candidate == id)
                .ok_or_else(|| {
                    paro_error::internal("scalar fingerprint bucket lost an arena node")
                })?;
            bucket.remove(position);
            if bucket.is_empty() {
                self.by_fingerprint.remove(&fingerprint);
            }
        }
        self.nodes.truncate(len);
        Ok(())
    }

    pub fn intern(&mut self, spec: ScalarSpec) -> Result<ScalarExprId> {
        self.validate_shape(&spec)?;
        let properties = self.derive_properties(&spec)?;
        let fingerprint = self.fingerprint(&spec)?;
        let node = ScalarNode {
            kind: spec.kind,
            logical_type: spec.logical_type,
            children: spec.children,
            properties,
            fingerprint,
        };
        if let Some(existing) = self.by_fingerprint.get(&fingerprint) {
            if let Some(id) = existing
                .iter()
                .copied()
                .find(|id| self.nodes[id.index()] == node)
            {
                return Ok(id);
            }
        }
        let id = ScalarExprId::new(self.nodes.len());
        self.nodes.push(node);
        self.by_fingerprint.entry(fingerprint).or_default().push(id);
        Ok(id)
    }

    pub fn canonical_comparison(
        &mut self,
        mut op: ComparisonOp,
        mut left: ScalarExprId,
        mut right: ScalarExprId,
    ) -> Result<ScalarExprId> {
        let left_node = self
            .get(left)
            .ok_or_else(|| paro_error::internal("unknown left scalar expression"))?;
        let right_node = self
            .get(right)
            .ok_or_else(|| paro_error::internal("unknown right scalar expression"))?;
        if left_node.logical_type != right_node.logical_type {
            return Err(paro_error::internal(
                "comparison operands must have normalized compatible types",
            ));
        }
        if left_node.properties.can_reorder_and_share()
            && right_node.properties.can_reorder_and_share()
            && op.can_swap()
            && (right_node.fingerprint, right) < (left_node.fingerprint, left)
        {
            std::mem::swap(&mut left, &mut right);
            op = op.swapped();
        }
        self.intern(ScalarSpec {
            kind: ScalarKind::Comparison(op),
            logical_type: LogicalType::Boolean,
            children: vec![left, right].into_boxed_slice(),
            local_properties: ScalarLocalProperties::default(),
        })
    }

    pub fn canonical_conjunction(
        &mut self,
        kind: ScalarKind,
        children: impl IntoIterator<Item = ScalarExprId>,
    ) -> Result<ScalarExprId> {
        if !matches!(kind, ScalarKind::And | ScalarKind::Or) {
            return Err(paro_error::internal(
                "canonical_conjunction requires AND or OR",
            ));
        }
        let mut flattened = Vec::new();
        for child in children {
            let child_node = self
                .get(child)
                .ok_or_else(|| paro_error::internal("unknown conjunction child"))?;
            if child_node.kind == kind && child_node.properties.can_reorder_and_share() {
                flattened.extend(child_node.children.iter().copied());
            } else {
                flattened.push(child);
            }
        }
        let can_canonicalize = flattened
            .iter()
            .all(|id| self.nodes[id.index()].properties.can_reorder_and_share());
        if can_canonicalize {
            flattened.sort_by_key(|id| (self.nodes[id.index()].fingerprint, *id));
            flattened.dedup();
        }
        self.intern(ScalarSpec {
            kind,
            logical_type: LogicalType::Boolean,
            children: flattened.into_boxed_slice(),
            local_properties: ScalarLocalProperties::default(),
        })
    }

    fn validate_shape(&self, spec: &ScalarSpec) -> Result<()> {
        if let ScalarKind::Constant { value } = &spec.kind {
            if value.logical_type() != &spec.logical_type {
                return Err(paro_error::internal(
                    "scalar literal type disagrees with its node",
                ));
            }
        }
        if spec.children.iter().any(|id| self.get(*id).is_none()) {
            return Err(paro_error::internal(
                "scalar node references a child outside its arena generation",
            ));
        }
        let arity_ok = match spec.kind {
            ScalarKind::Constant { .. } | ScalarKind::Column(_) | ScalarKind::Parameter(_) => {
                spec.children.is_empty()
            }
            ScalarKind::CorrelatedColumn { depth, .. } => depth > 0 && spec.children.is_empty(),
            ScalarKind::Cast { .. } => spec.children.len() == 1,
            ScalarKind::Comparison(_) => spec.children.len() == 2,
            ScalarKind::Case => spec.children.len() == 3,
            ScalarKind::And | ScalarKind::Or => !spec.children.is_empty(),
            ScalarKind::Function { .. }
            | ScalarKind::Operator { .. }
            | ScalarKind::Aggregate { .. }
            | ScalarKind::BoundSubquery { .. }
            | ScalarKind::Window { .. } => true,
        };
        if !arity_ok {
            return Err(paro_error::internal("invalid normalized scalar arity"));
        }
        Ok(())
    }

    fn derive_properties(&self, spec: &ScalarSpec) -> Result<ScalarProperties> {
        let mut referenced_columns = BTreeSet::new();
        if let ScalarKind::Column(column) | ScalarKind::CorrelatedColumn { column, .. } = spec.kind
        {
            referenced_columns.insert(column);
        }
        let mut volatility = spec.local_properties.volatility;
        let mut may_error = spec.local_properties.may_error;
        let mut has_side_effects = spec.local_properties.has_side_effects;
        let mut depends_on_external_state = spec.local_properties.depends_on_external_state;
        let mut deterministic = spec.local_properties.deterministic;
        for id in &spec.children {
            let child = self
                .get(*id)
                .ok_or_else(|| paro_error::internal("unknown scalar child"))?;
            referenced_columns.extend(child.properties.referenced_columns.iter().copied());
            volatility = volatility.max(child.properties.volatility);
            may_error |= child.properties.may_error;
            has_side_effects |= child.properties.has_side_effects;
            depends_on_external_state |= child.properties.depends_on_external_state;
            deterministic &= child.properties.deterministic;
        }
        deterministic &=
            volatility != Volatility::Volatile && !has_side_effects && !depends_on_external_state;
        Ok(ScalarProperties {
            referenced_columns,
            volatility,
            may_error,
            has_side_effects,
            depends_on_external_state,
            deterministic,
        })
    }

    fn fingerprint(&self, spec: &ScalarSpec) -> Result<Fingerprint> {
        let mut builder = StableFingerprintBuilder::default();
        encode_kind(&mut builder, &spec.kind);
        encode_logical_type(&mut builder, &spec.logical_type);
        builder.write_u64(spec.local_properties.volatility as u64);
        builder.write_u64(spec.local_properties.may_error as u64);
        builder.write_u64(spec.local_properties.has_side_effects as u64);
        builder.write_u64(spec.local_properties.depends_on_external_state as u64);
        builder.write_u64(spec.local_properties.deterministic as u64);
        for child in &spec.children {
            let fingerprint = self
                .get(*child)
                .ok_or_else(|| paro_error::internal("unknown scalar child"))?
                .fingerprint;
            builder.write_fingerprint(fingerprint);
        }
        Ok(builder.finish())
    }
}

fn encode_kind(builder: &mut StableFingerprintBuilder, kind: &ScalarKind) {
    let tag = match kind {
        ScalarKind::Constant { .. } => 0,
        ScalarKind::Column(_) => 1,
        ScalarKind::Parameter(_) => 2,
        ScalarKind::Function { .. } => 3,
        ScalarKind::Cast { .. } => 4,
        ScalarKind::And => 5,
        ScalarKind::Or => 6,
        ScalarKind::Comparison(_) => 7,
        ScalarKind::Case => 8,
        ScalarKind::Operator { .. } => 9,
        ScalarKind::Aggregate { .. } => 10,
        ScalarKind::BoundSubquery { .. } => 11,
        ScalarKind::Window { .. } => 12,
        ScalarKind::CorrelatedColumn { .. } => 13,
    };
    builder.write_u64(tag);
    match kind {
        ScalarKind::Constant { value } => builder.write_fingerprint(value.fingerprint()),
        ScalarKind::Function { routine: value }
        | ScalarKind::Operator { operator: value }
        | ScalarKind::Aggregate { function: value }
        | ScalarKind::BoundSubquery { query: value }
        | ScalarKind::Window { function: value } => builder.write_fingerprint(*value),
        ScalarKind::Column(column) => builder.write_u64(column.0 as u64),
        ScalarKind::CorrelatedColumn { column, depth } => {
            builder.write_u64(column.0 as u64);
            builder.write_u64(*depth as u64);
        }
        ScalarKind::Parameter(slot) => builder.write_u64(*slot as u64),
        ScalarKind::Comparison(op) => builder.write_u64(*op as u64),
        ScalarKind::Cast { try_cast } => builder.write_u64(*try_cast as u64),
        ScalarKind::And | ScalarKind::Or | ScalarKind::Case => {}
    }
}

pub(crate) fn encode_logical_type(builder: &mut StableFingerprintBuilder, ty: &LogicalType) {
    // Logical types can be nested independently of the expression tree (for
    // example, a deeply nested LIST/STRUCT literal).  Keep this identity
    // encoder stack-safe as well; expression_fingerprint and value_fingerprint
    // both call it on their hot cache-key paths.
    enum Task<'a> {
        Type(&'a LogicalType),
        StructName(&'a str),
        U64(u64),
    }

    let mut pending = vec![Task::Type(ty)];
    while let Some(task) = pending.pop() {
        match task {
            Task::Type(ty) => {
                builder.write_u64(ty.type_id() as u64);
                match ty {
                    LogicalType::Decimal { precision, scale } => {
                        builder.write_u64(*precision as u64);
                        builder.write_u64(*scale as u64);
                    }
                    LogicalType::VarcharCollation(collation) => {
                        builder.write_bytes(collation.as_bytes())
                    }
                    LogicalType::IntegerLiteral(value) => builder.write_u64(*value as u64),
                    LogicalType::Array(child, length) => {
                        // The child encoding precedes the fixed length.
                        pending.push(Task::U64(*length as u64));
                        pending.push(Task::Type(child));
                    }
                    LogicalType::List(child) => pending.push(Task::Type(child)),
                    LogicalType::Struct(fields) => {
                        builder.write_u64(fields.len() as u64);
                        // Push in reverse so each field is encoded as
                        // name, type, matching the historical wire format.
                        for (name, field) in fields.iter().rev() {
                            pending.push(Task::Type(field));
                            pending.push(Task::StructName(name));
                        }
                    }
                    _ => {}
                }
            }
            Task::StructName(name) => builder.write_bytes(name.as_bytes()),
            Task::U64(value) => builder.write_u64(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(arena: &mut ScalarArena, id: u32) -> ScalarExprId {
        arena
            .intern(ScalarSpec {
                kind: ScalarKind::Column(ColumnId(id)),
                logical_type: LogicalType::BigInt,
                children: Box::new([]),
                local_properties: ScalarLocalProperties::default(),
            })
            .unwrap()
    }

    #[test]
    fn semantic_duplicates_are_hash_consed() {
        let mut arena = ScalarArena::default();
        let first = column(&mut arena, 1);
        let second = column(&mut arena, 1);
        assert_eq!(first, second);
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn safe_comparison_is_canonical_but_error_fence_is_not_reordered() {
        let mut arena = ScalarArena::default();
        let a = column(&mut arena, 1);
        let b = column(&mut arena, 2);
        let ab = arena
            .canonical_comparison(ComparisonOp::Equal, a, b)
            .unwrap();
        let ba = arena
            .canonical_comparison(ComparisonOp::Equal, b, a)
            .unwrap();
        assert_eq!(ab, ba);

        let risky = arena
            .intern(ScalarSpec {
                kind: ScalarKind::Function {
                    routine: Fingerprint(99),
                },
                logical_type: LogicalType::BigInt,
                children: Box::new([]),
                local_properties: ScalarLocalProperties {
                    may_error: true,
                    ..Default::default()
                },
            })
            .unwrap();
        let left_first = arena
            .canonical_comparison(ComparisonOp::Equal, risky, a)
            .unwrap();
        let risky_first = arena
            .canonical_comparison(ComparisonOp::Equal, a, risky)
            .unwrap();
        assert_ne!(left_first, risky_first);
    }

    #[test]
    fn conjunction_flattens_and_deduplicates_only_without_fences() {
        let mut arena = ScalarArena::default();
        let a = column(&mut arena, 1);
        let b = column(&mut arena, 2);
        let nested = arena
            .canonical_conjunction(ScalarKind::And, [a, b])
            .unwrap();
        let flattened = arena
            .canonical_conjunction(ScalarKind::And, [b, nested, a])
            .unwrap();
        assert_eq!(arena.get(flattened).unwrap().children.len(), 2);
    }
}
