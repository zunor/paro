// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical expression program image and cache.
//!
//! EXPR-PROGRAM v1 intentionally uses a Velox-style vectorized expression tree with
//! typed kernel dispatch. It does not introduce bytecode or JIT; those can be
//! separate backends behind the same program/cache versioning later.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use paro_common::runtime_value::Value;
use paro_common::typed_parameters::ParameterSlot;
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_function::scalar::cast::BoundCastInfo;
use paro_function::scalar::operators::arithmetic::{
    try_decimal_factor_fusion, try_decimal_factor_product_fusion, DecimalOperandSide,
};
use paro_function::scalar::{BoundScalarFunction, FunctionSideEffects, FunctionStability};
use paro_planner::expression::{
    ComparisonType, ConjunctionType, Expression, ExpressionIterator, ExpressionVisitDecision,
    OperatorType,
};

mod fingerprint;
mod fusion;
mod identity;

use fingerprint::ExpressionFingerprintCatalog;
pub use fingerprint::{expression_fingerprint, expression_list_fingerprints};
use fusion::compile_decimal_factor_chains;
pub use fusion::PhysicalDecimalFactorChain;
use identity::{
    ExpressionIdentity, ExpressionIdentityRef, ExpressionIdentityRefMap, ExpressionIdentityRefSet,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExpressionBackend {
    VectorTreeV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpressionProgramVersion {
    pub backend: ExpressionBackend,
    pub physical_semantics_version: u32,
    pub visible_generation: u64,
    pub settings_fingerprint: u64,
}

impl ExpressionProgramVersion {
    pub fn anonymous() -> Self {
        Self {
            backend: ExpressionBackend::VectorTreeV1,
            physical_semantics_version: 1,
            visible_generation: 0,
            settings_fingerprint: 0,
        }
    }

    pub fn from_session(session: &StatementContext) -> Self {
        let env = session.compile_environment_key();
        Self {
            backend: ExpressionBackend::VectorTreeV1,
            physical_semantics_version: 1,
            visible_generation: env.visible_generation,
            settings_fingerprint: env.settings_fingerprint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExpressionProgramCacheBucketKey {
    root_fingerprints: Box<[u64]>,
    retained_identity_nodes: usize,
    backend: ExpressionBackend,
    physical_semantics_version: u32,
    visible_generation: u64,
    settings_fingerprint: u64,
}

impl ExpressionProgramCacheBucketKey {
    fn new(exprs: &[Expression], version: &ExpressionProgramVersion) -> Self {
        Self::from_expressions(exprs.iter(), version)
    }

    fn from_expressions<'a>(
        exprs: impl Iterator<Item = &'a Expression> + Clone,
        version: &ExpressionProgramVersion,
    ) -> Self {
        let catalog = ExpressionFingerprintCatalog::from_expressions(exprs.clone());
        let retained_identity_nodes = catalog.retained_nodes(exprs.clone());
        let root_fingerprints = exprs
            .map(|expression| catalog.fingerprint(expression))
            .collect::<Vec<_>>();
        Self {
            root_fingerprints: root_fingerprints.into_boxed_slice(),
            retained_identity_nodes,
            backend: version.backend,
            physical_semantics_version: version.physical_semantics_version,
            visible_generation: version.visible_generation,
            settings_fingerprint: version.settings_fingerprint,
        }
    }
}

const DEFAULT_PROGRAM_CACHE_LIMIT: usize = 4096;
const DEFAULT_PROGRAM_CACHE_IDENTITY_NODE_LIMIT: usize = 262_144;

#[derive(Debug)]
pub struct ExpressionProgramCache {
    programs: HashMap<ExpressionProgramCacheBucketKey, Vec<CachedProgramEntry>>,
    lru: VecDeque<(ExpressionProgramCacheBucketKey, u64, u64)>,
    entry_count: usize,
    retained_identity_nodes: usize,
    next_entry_id: u64,
    max_entries: usize,
    max_identity_nodes: usize,
    access_epoch: u64,
    hits: u64,
    misses: u64,
}

#[derive(Debug)]
struct CachedProgramEntry {
    id: u64,
    root_identities: Box<[ExpressionIdentity]>,
    retained_identity_nodes: usize,
    program: Arc<PhysicalExpressionProgram>,
    epoch: u64,
}

impl Default for ExpressionProgramCache {
    fn default() -> Self {
        Self::with_limits(
            DEFAULT_PROGRAM_CACHE_LIMIT,
            DEFAULT_PROGRAM_CACHE_IDENTITY_NODE_LIMIT,
        )
    }
}

impl ExpressionProgramCache {
    pub fn with_capacity_limit(max_entries: usize) -> Self {
        Self::with_limits(max_entries, max_entries.saturating_mul(64).max(1))
    }

    pub fn with_limits(max_entries: usize, max_identity_nodes: usize) -> Self {
        Self {
            programs: HashMap::new(),
            lru: VecDeque::new(),
            entry_count: 0,
            retained_identity_nodes: 0,
            next_entry_id: 0,
            max_entries: max_entries.max(1),
            max_identity_nodes: max_identity_nodes.max(1),
            access_epoch: 0,
            hits: 0,
            misses: 0,
        }
    }

    pub fn get_or_compile(
        &mut self,
        exprs: &[Expression],
        version: ExpressionProgramVersion,
    ) -> Arc<PhysicalExpressionProgram> {
        let key = ExpressionProgramCacheBucketKey::new(exprs, &version);
        let identities = exprs
            .iter()
            .zip(key.root_fingerprints.iter().copied())
            .map(|(expression, fingerprint)| ExpressionIdentityRef::new(expression, fingerprint))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        self.get_or_compile_with(key, identities, |root_fingerprints| {
            PhysicalExpressionProgram::compile_with_fingerprints(exprs, version, root_fingerprints)
        })
    }

    pub(crate) fn get_or_compile_refs(
        &mut self,
        exprs: &[&Expression],
        version: ExpressionProgramVersion,
    ) -> Arc<PhysicalExpressionProgram> {
        let key =
            ExpressionProgramCacheBucketKey::from_expressions(exprs.iter().copied(), &version);
        let identities = exprs
            .iter()
            .zip(key.root_fingerprints.iter().copied())
            .map(|(expression, fingerprint)| ExpressionIdentityRef::new(expression, fingerprint))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        self.get_or_compile_with(key, identities, |root_fingerprints| {
            PhysicalExpressionProgram::compile_refs_with_fingerprints(
                exprs,
                version,
                root_fingerprints,
            )
        })
    }

    fn get_or_compile_with(
        &mut self,
        key: ExpressionProgramCacheBucketKey,
        root_identities: Box<[ExpressionIdentityRef<'_>]>,
        compile: impl FnOnce(Vec<u64>) -> PhysicalExpressionProgram,
    ) -> Arc<PhysicalExpressionProgram> {
        let epoch = self.next_epoch();
        if let Some(entry) = self.programs.get_mut(&key).and_then(|bucket| {
            bucket
                .iter_mut()
                .find(|entry| identities_match(&entry.root_identities, &root_identities))
        }) {
            self.hits += 1;
            entry.epoch = epoch;
            let entry_id = entry.id;
            let program = Arc::clone(&entry.program);
            self.lru.push_back((key, entry_id, epoch));
            self.compact_stale_lru_if_needed();
            self.debug_assert_accounting();
            return program;
        }
        self.misses += 1;
        let program = Arc::new(compile(key.root_fingerprints.to_vec()));
        if key.retained_identity_nodes > self.max_identity_nodes {
            self.debug_assert_accounting();
            return program;
        }
        self.next_entry_id = self.next_entry_id.wrapping_add(1).max(1);
        let entry_id = self.next_entry_id;
        self.programs
            .entry(key.clone())
            .or_default()
            .push(CachedProgramEntry {
                id: entry_id,
                root_identities: root_identities
                    .iter()
                    .copied()
                    .map(ExpressionIdentity::snapshot)
                    .collect(),
                retained_identity_nodes: key.retained_identity_nodes,
                program: Arc::clone(&program),
                epoch,
            });
        self.entry_count += 1;
        self.retained_identity_nodes = self
            .retained_identity_nodes
            .saturating_add(key.retained_identity_nodes);
        self.lru.push_back((key, entry_id, epoch));
        self.evict_over_limit();
        self.compact_stale_lru_if_needed();
        self.debug_assert_accounting();
        program
    }

    fn next_epoch(&mut self) -> u64 {
        self.access_epoch = self.access_epoch.wrapping_add(1).max(1);
        self.access_epoch
    }

    fn evict_over_limit(&mut self) {
        while self.entry_count > self.max_entries
            || self.retained_identity_nodes > self.max_identity_nodes
        {
            let Some((victim, entry_id, epoch)) = self.lru.pop_front() else {
                break;
            };
            let mut remove_bucket = false;
            if let Some(bucket) = self.programs.get_mut(&victim) {
                if let Some(index) = bucket
                    .iter()
                    .position(|entry| entry.id == entry_id && entry.epoch == epoch)
                {
                    let removed = bucket.swap_remove(index);
                    self.entry_count -= 1;
                    self.retained_identity_nodes = self
                        .retained_identity_nodes
                        .saturating_sub(removed.retained_identity_nodes);
                }
                remove_bucket = bucket.is_empty();
            }
            if remove_bucket {
                self.programs.remove(&victim);
            }
        }
    }

    fn compact_stale_lru_if_needed(&mut self) {
        if self.lru.len() <= self.max_entries.saturating_mul(4).max(16) {
            return;
        }
        let mut entries = Vec::with_capacity(self.entry_count);
        for (key, bucket) in &self.programs {
            for entry in bucket {
                entries.push((key.clone(), entry.id, entry.epoch));
            }
        }
        entries.sort_unstable_by_key(|(_, _, epoch)| *epoch);
        self.lru = entries.into();
    }

    fn debug_assert_accounting(&self) {
        debug_assert_eq!(
            self.entry_count,
            self.programs.values().map(Vec::len).sum::<usize>(),
            "expression program cache entry accounting diverged from bucket storage"
        );
        debug_assert_eq!(
            self.retained_identity_nodes,
            self.programs
                .values()
                .flatten()
                .map(|entry| entry.retained_identity_nodes)
                .sum::<usize>(),
            "expression program cache identity-node accounting diverged from bucket storage"
        );
    }

    #[cfg(test)]
    pub fn contains_program(
        &self,
        exprs: &[Expression],
        version: &ExpressionProgramVersion,
    ) -> bool {
        let key = ExpressionProgramCacheBucketKey::new(exprs, version);
        let identities = exprs
            .iter()
            .zip(key.root_fingerprints.iter().copied())
            .map(|(expression, fingerprint)| ExpressionIdentityRef::new(expression, fingerprint))
            .collect::<Vec<_>>();
        self.programs.get(&key).is_some_and(|bucket| {
            bucket
                .iter()
                .any(|entry| identities_match(&entry.root_identities, &identities))
        })
    }

    #[cfg(test)]
    pub fn hits(&self) -> u64 {
        self.hits
    }

    #[cfg(test)]
    pub fn misses(&self) -> u64 {
        self.misses
    }

    pub fn len(&self) -> usize {
        self.entry_count
    }
}

fn identities_match(owned: &[ExpressionIdentity], borrowed: &[ExpressionIdentityRef<'_>]) -> bool {
    owned.len() == borrowed.len()
        && owned
            .iter()
            .zip(borrowed)
            .all(|(owned, borrowed)| owned.matches(*borrowed))
}

#[derive(Debug, Clone)]
pub struct PhysicalExpressionProgram {
    roots: Vec<PhysicalExpression>,
    shared_nodes: Vec<PhysicalExpression>,
    decimal_factor_chains: Vec<PhysicalDecimalFactorChain>,
    scratch_layout: ExpressionScratchLayout,
    root_to_unique: Vec<usize>,
    root_first_output: Vec<usize>,
    root_fingerprints: Box<[u64]>,
    version: ExpressionProgramVersion,
}

impl PhysicalExpressionProgram {
    pub fn compile(exprs: &[Expression], version: ExpressionProgramVersion) -> Self {
        Self::compile_with_fingerprints(exprs, version, expression_list_fingerprints(exprs))
    }

    fn compile_with_fingerprints(
        exprs: &[Expression],
        version: ExpressionProgramVersion,
        root_fingerprints: Vec<u64>,
    ) -> Self {
        Self::compile_iter(exprs.iter(), exprs.len(), version, root_fingerprints)
    }

    fn compile_refs_with_fingerprints(
        exprs: &[&Expression],
        version: ExpressionProgramVersion,
        root_fingerprints: Vec<u64>,
    ) -> Self {
        Self::compile_iter(
            exprs.iter().copied(),
            exprs.len(),
            version,
            root_fingerprints,
        )
    }

    fn compile_iter<'a, I>(
        exprs: I,
        root_count: usize,
        version: ExpressionProgramVersion,
        root_fingerprints: Vec<u64>,
    ) -> Self
    where
        I: Iterator<Item = &'a Expression> + Clone,
    {
        let exprs = exprs.collect::<Vec<_>>();
        let fingerprints = ExpressionFingerprintCatalog::from_expressions(exprs.iter().copied());
        let shared_candidates = SharedExpressionCandidates::from_expressions(&exprs, &fingerprints);
        let mut compiler = ProgramCompiler::new(shared_candidates, &fingerprints);
        let mut root_to_unique = Vec::with_capacity(root_count);
        let mut root_first_output = Vec::with_capacity(root_count);
        let mut unique_by_identity = ExpressionIdentityRefMap::default();
        let mut roots = Vec::new();

        debug_assert_eq!(root_count, root_fingerprints.len());
        for expr in exprs {
            let compiled = compiler.compile_expression(expr);
            if compiled.cse_safe {
                let unique =
                    *unique_by_identity.get_or_insert_with(fingerprints.identity(expr), || {
                        roots.push(compiled.expr.clone());
                        roots.len() - 1
                    });
                root_to_unique.push(unique);
                let first = root_to_unique
                    .iter()
                    .position(|&candidate| candidate == unique)
                    .unwrap_or(root_to_unique.len() - 1);
                root_first_output.push(first);
            } else {
                roots.push(compiled.expr);
                let unique = roots.len() - 1;
                root_to_unique.push(unique);
                root_first_output.push(root_to_unique.len() - 1);
            }
        }

        let shared_nodes = compiler
            .shared_nodes
            .into_iter()
            .map(|node| node.expect("shared expression candidate was not compiled"))
            .collect::<Vec<_>>();
        let decimal_factor_chains = compile_decimal_factor_chains(
            &roots,
            &shared_nodes,
            &root_to_unique,
            &root_first_output,
        );
        Self {
            roots,
            shared_nodes,
            decimal_factor_chains,
            scratch_layout: ExpressionScratchLayout {
                slots: compiler.scratch_slots.into_boxed_slice(),
            },
            root_to_unique,
            root_first_output,
            root_fingerprints: root_fingerprints.into_boxed_slice(),
            version,
        }
    }

    #[inline]
    pub fn root_count(&self) -> usize {
        self.root_to_unique.len()
    }

    #[inline]
    pub fn unique_root_count(&self) -> usize {
        self.roots.len()
    }

    #[inline]
    pub fn unique_root(&self, unique_idx: usize) -> &PhysicalExpression {
        &self.roots[unique_idx]
    }

    #[inline]
    pub fn shared_expression_count(&self) -> usize {
        self.shared_nodes.len()
    }

    #[inline]
    pub fn root(&self, expr_idx: usize) -> &PhysicalExpression {
        &self.roots[self.root_to_unique[expr_idx]]
    }

    #[inline]
    pub fn shared_node(&self, slot: usize) -> &PhysicalExpression {
        &self.shared_nodes[slot]
    }

    #[inline]
    pub fn shared_nodes(&self) -> &[PhysicalExpression] {
        &self.shared_nodes
    }

    #[inline]
    pub fn decimal_factor_chains(&self) -> &[PhysicalDecimalFactorChain] {
        &self.decimal_factor_chains
    }

    #[inline]
    pub fn scratch_layout(&self) -> &ExpressionScratchLayout {
        &self.scratch_layout
    }

    #[inline]
    pub fn root_state_index(&self, expr_idx: usize) -> usize {
        self.root_to_unique[expr_idx]
    }

    #[inline]
    pub fn root_first_output(&self, expr_idx: usize) -> usize {
        self.root_first_output[expr_idx]
    }

    #[inline]
    pub fn root_return_type(&self, expr_idx: usize) -> LogicalType {
        self.root(expr_idx).return_type()
    }

    #[inline]
    pub fn root_return_types(&self) -> Vec<LogicalType> {
        (0..self.root_count())
            .map(|idx| self.root_return_type(idx))
            .collect()
    }

    #[inline]
    pub fn version(&self) -> &ExpressionProgramVersion {
        &self.version
    }

    pub fn root_fingerprints(&self) -> &[u64] {
        &self.root_fingerprints
    }
}

#[derive(Debug, Clone)]
pub enum PhysicalExpression {
    Function(PhysicalFunctionExpression),
    Cast(PhysicalCastExpression),
    Comparison(PhysicalComparisonExpression),
    Conjunction(PhysicalConjunctionExpression),
    Case(PhysicalCaseExpression),
    Operator(PhysicalOperatorExpression),
    Constant(ExpressionConstant),
    Parameter(PhysicalParameterExpression),
    ColumnRef(PhysicalColumnRefExpression),
    Reference(PhysicalReferenceExpression),
    Shared(PhysicalSharedExpression),
}

impl PhysicalExpression {
    pub fn return_type(&self) -> LogicalType {
        match self {
            Self::Function(expr) => expr.return_type.clone(),
            Self::Cast(expr) => expr.target_type.clone(),
            Self::Comparison(_) | Self::Conjunction(_) => LogicalType::Boolean,
            Self::Case(expr) => expr.return_type.clone(),
            Self::Operator(expr) => expr.return_type.clone(),
            Self::Constant(expr) => expr.return_type.clone(),
            Self::Parameter(expr) => expr.slot.ty.clone(),
            Self::ColumnRef(expr) => expr.return_type.clone(),
            Self::Reference(expr) => expr.return_type.clone(),
            Self::Shared(expr) => expr.return_type.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExpressionScratchLayout {
    slots: Box<[ExpressionScratchSlot]>,
}

impl ExpressionScratchLayout {
    #[inline]
    pub fn slots(&self) -> &[ExpressionScratchSlot] {
        &self.slots
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct ExpressionScratchSlot {
    pub return_type: LogicalType,
}

#[derive(Debug, Clone)]
pub struct PhysicalFunctionExpression {
    pub function: BoundScalarFunction,
    pub children: Vec<PhysicalExpression>,
    pub return_type: LogicalType,
}

#[derive(Debug, Clone)]
pub struct PhysicalCastExpression {
    pub child: Box<PhysicalExpression>,
    pub target_type: LogicalType,
    pub try_cast: bool,
    pub cast_info: BoundCastInfo,
}

#[derive(Debug, Clone)]
pub struct PhysicalComparisonExpression {
    pub left: Box<PhysicalExpression>,
    pub right: Box<PhysicalExpression>,
    pub comparison_type: ComparisonType,
    pub left_type: LogicalType,
}

#[derive(Debug, Clone)]
pub struct PhysicalConjunctionExpression {
    pub conjunction_type: ConjunctionType,
    pub children: Vec<PhysicalExpression>,
}

#[derive(Debug, Clone)]
pub struct PhysicalCaseExpression {
    pub check: Box<PhysicalExpression>,
    pub result_if_true: Box<PhysicalExpression>,
    pub result_if_false: Box<PhysicalExpression>,
    pub return_type: LogicalType,
}

#[derive(Debug, Clone)]
pub struct PhysicalOperatorExpression {
    pub operator_type: OperatorType,
    pub children: Vec<PhysicalExpression>,
    pub return_type: LogicalType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpressionConstant {
    pub value: Value,
    pub return_type: LogicalType,
}

#[derive(Debug, Clone)]
pub struct PhysicalParameterExpression {
    pub slot: ParameterSlot,
}

#[derive(Debug, Clone)]
pub struct PhysicalColumnRefExpression {
    pub column_index: usize,
    pub return_type: LogicalType,
}

#[derive(Debug, Clone)]
pub struct PhysicalReferenceExpression {
    pub index: usize,
    pub return_type: LogicalType,
}

#[derive(Debug, Clone)]
pub struct PhysicalSharedExpression {
    pub slot: usize,
    pub return_type: LogicalType,
}

struct ProgramCompiler<'a> {
    fingerprints: &'a ExpressionFingerprintCatalog,
    shared_slots_by_identity: ExpressionIdentityRefMap<'a, usize>,
    shared_nodes: Vec<Option<PhysicalExpression>>,
    scratch_slots: Vec<ExpressionScratchSlot>,
    compiling_shared: HashSet<usize>,
}

struct CompiledExpr {
    expr: PhysicalExpression,
    cse_safe: bool,
}

impl<'a> ProgramCompiler<'a> {
    fn new(
        shared_candidates: SharedExpressionCandidates<'a>,
        fingerprints: &'a ExpressionFingerprintCatalog,
    ) -> Self {
        Self {
            fingerprints,
            shared_slots_by_identity: shared_candidates.slots_by_identity,
            shared_nodes: vec![None; shared_candidates.slots.len()],
            scratch_slots: shared_candidates.slots,
            compiling_shared: HashSet::new(),
        }
    }

    fn compile_expression(&mut self, expr: &'a Expression) -> CompiledExpr {
        let identity = self.fingerprints.identity(expr);
        if let Some(&slot) = self.shared_slots_by_identity.get(identity) {
            if !self.compiling_shared.contains(&slot) {
                if self.shared_nodes[slot].is_none() {
                    self.compiling_shared.insert(slot);
                    let compiled = self.compile_expression_inner(expr);
                    self.compiling_shared.remove(&slot);
                    self.shared_nodes[slot] = Some(compiled.expr);
                }
                return CompiledExpr {
                    expr: PhysicalExpression::Shared(PhysicalSharedExpression {
                        slot,
                        return_type: expr.return_type(),
                    }),
                    cse_safe: true,
                };
            }
        }
        self.compile_expression_inner(expr)
    }

    fn compile_expression_inner(&mut self, expr: &'a Expression) -> CompiledExpr {
        match expr {
            Expression::Function(expr) => {
                if let Some(fused) = self.try_compile_decimal_factor_product_fusion(expr) {
                    return fused;
                }
                if let Some(fused) = self.try_compile_decimal_factor_fusion(expr) {
                    return fused;
                }
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.compile_expression(child))
                    .collect::<Vec<_>>();
                let cse_safe = children.iter().all(|child| child.cse_safe)
                    && expr.function.stability == FunctionStability::Consistent
                    && expr.function.side_effects == FunctionSideEffects::NoSideEffects;
                let physical = PhysicalExpression::Function(PhysicalFunctionExpression {
                    function: expr.function.clone(),
                    children: children.into_iter().map(|child| child.expr).collect(),
                    return_type: expr.return_type.clone(),
                });
                CompiledExpr {
                    expr: physical,
                    cse_safe,
                }
            }
            Expression::Cast(expr) => {
                let child = self.compile_expression(&expr.child);
                let physical = PhysicalExpression::Cast(PhysicalCastExpression {
                    child: Box::new(child.expr),
                    target_type: expr.target_type.clone(),
                    try_cast: expr.try_cast,
                    cast_info: expr.cast_info.clone(),
                });
                CompiledExpr {
                    expr: physical,
                    cse_safe: child.cse_safe,
                }
            }
            Expression::Comparison(expr) => {
                let left = self.compile_expression(&expr.left);
                let right = self.compile_expression(&expr.right);
                let left_type = expr.left.return_type();
                let physical = PhysicalExpression::Comparison(PhysicalComparisonExpression {
                    left: Box::new(left.expr),
                    right: Box::new(right.expr),
                    comparison_type: expr.comparison_type,
                    left_type,
                });
                CompiledExpr {
                    expr: physical,
                    cse_safe: left.cse_safe && right.cse_safe,
                }
            }
            Expression::Conjunction(expr) => {
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.compile_expression(child))
                    .collect::<Vec<_>>();
                let physical = PhysicalExpression::Conjunction(PhysicalConjunctionExpression {
                    conjunction_type: expr.conjunction_type,
                    children: children.iter().map(|child| child.expr.clone()).collect(),
                });
                CompiledExpr {
                    expr: physical,
                    cse_safe: children.iter().all(|child| child.cse_safe),
                }
            }
            Expression::Case(expr) => {
                let check = self.compile_expression(&expr.check);
                let if_true = self.compile_expression(&expr.result_if_true);
                let if_false = self.compile_expression(&expr.result_if_false);
                let physical = PhysicalExpression::Case(PhysicalCaseExpression {
                    check: Box::new(check.expr),
                    result_if_true: Box::new(if_true.expr),
                    result_if_false: Box::new(if_false.expr),
                    return_type: expr.return_type.clone(),
                });
                CompiledExpr {
                    expr: physical,
                    cse_safe: check.cse_safe && if_true.cse_safe && if_false.cse_safe,
                }
            }
            Expression::Operator(expr) => {
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.compile_expression(child))
                    .collect::<Vec<_>>();
                let physical = PhysicalExpression::Operator(PhysicalOperatorExpression {
                    operator_type: expr.operator_type,
                    children: children.iter().map(|child| child.expr.clone()).collect(),
                    return_type: expr.return_type.clone(),
                });
                CompiledExpr {
                    expr: physical,
                    cse_safe: children.iter().all(|child| child.cse_safe),
                }
            }
            Expression::Constant(expr) => {
                let constant = ExpressionConstant {
                    value: expr.value.clone(),
                    return_type: expr.return_type.clone(),
                };
                CompiledExpr {
                    expr: PhysicalExpression::Constant(constant),
                    cse_safe: true,
                }
            }
            Expression::Parameter(expr) => CompiledExpr {
                expr: PhysicalExpression::Parameter(PhysicalParameterExpression {
                    slot: expr.slot.clone(),
                }),
                cse_safe: false,
            },
            Expression::ColumnRef(expr) => CompiledExpr {
                expr: PhysicalExpression::ColumnRef(PhysicalColumnRefExpression {
                    column_index: expr.binding.column_index,
                    return_type: expr.return_type.clone(),
                }),
                cse_safe: true,
            },
            Expression::Reference(expr) => CompiledExpr {
                expr: PhysicalExpression::Reference(PhysicalReferenceExpression {
                    index: expr.index,
                    return_type: expr.return_type.clone(),
                }),
                cse_safe: true,
            },
            Expression::Aggregate(_) => {
                panic!("Aggregate expressions should not be compiled by ExpressionExecutor");
            }
            Expression::Subquery(_) => {
                panic!(
                    "ExpressionExecutor invariant violated: Expression::Subquery must be flattened before physical expression compilation"
                );
            }
            Expression::Window(_) => {
                panic!(
                    "ExpressionExecutor invariant violated: window expressions must be lowered to the window runtime"
                );
            }
        }
    }

    fn try_compile_decimal_factor_fusion(
        &mut self,
        expr: &'a paro_planner::expression::FunctionExpression,
    ) -> Option<CompiledExpr> {
        let (function, inputs) = self.try_bind_decimal_factor_fusion(expr)?;
        let children = inputs.map(|input| self.compile_expression(input));
        let cse_safe = children.iter().all(|child| child.cse_safe);
        Some(CompiledExpr {
            expr: PhysicalExpression::Function(PhysicalFunctionExpression {
                function,
                children: children.into_iter().map(|child| child.expr).collect(),
                return_type: expr.return_type.clone(),
            }),
            cse_safe,
        })
    }

    fn try_compile_decimal_factor_product_fusion(
        &mut self,
        expr: &'a paro_planner::expression::FunctionExpression,
    ) -> Option<CompiledExpr> {
        if expr.children.len() != 2
            || expr.function.stability != FunctionStability::Consistent
            || expr.function.side_effects != FunctionSideEffects::NoSideEffects
        {
            return None;
        }
        for factor_side in [DecimalOperandSide::Left, DecimalOperandSide::Right] {
            let factor_idx = usize::from(factor_side == DecimalOperandSide::Right);
            let product_idx = usize::from(factor_side == DecimalOperandSide::Left);
            let Expression::Function(factor_expression) = &expr.children[factor_idx] else {
                continue;
            };
            let Expression::Function(product_expression) = &expr.children[product_idx] else {
                continue;
            };
            if product_expression.children.len() != 2
                || product_expression.function.stability != FunctionStability::Consistent
                || product_expression.function.side_effects != FunctionSideEffects::NoSideEffects
                || self
                    .shared_slots_by_identity
                    .contains(self.fingerprints.identity(&expr.children[factor_idx]))
                || self
                    .shared_slots_by_identity
                    .contains(self.fingerprints.identity(&expr.children[product_idx]))
            {
                continue;
            }
            let Some((factor_function, factor_inputs)) =
                self.try_bind_decimal_factor_fusion(factor_expression)
            else {
                continue;
            };
            let Some(function) = try_decimal_factor_product_fusion(
                &expr.function,
                &factor_function,
                &product_expression.function,
                &product_expression.children[0].return_type(),
                &product_expression.children[1].return_type(),
                factor_side,
            ) else {
                continue;
            };
            let inputs = [
                factor_inputs[0],
                factor_inputs[1],
                &product_expression.children[0],
                &product_expression.children[1],
            ];
            let children = inputs.map(|input| self.compile_expression(input));
            let cse_safe = children.iter().all(|child| child.cse_safe);
            return Some(CompiledExpr {
                expr: PhysicalExpression::Function(PhysicalFunctionExpression {
                    function,
                    children: children.into_iter().map(|child| child.expr).collect(),
                    return_type: expr.return_type.clone(),
                }),
                cse_safe,
            });
        }
        None
    }

    fn try_bind_decimal_factor_fusion(
        &self,
        expr: &'a paro_planner::expression::FunctionExpression,
    ) -> Option<(BoundScalarFunction, [&'a Expression; 2])> {
        if expr.children.len() != 2
            || expr.function.stability != FunctionStability::Consistent
            || expr.function.side_effects != FunctionSideEffects::NoSideEffects
        {
            return None;
        }
        for nested_side in [DecimalOperandSide::Left, DecimalOperandSide::Right] {
            let nested_idx = usize::from(nested_side == DecimalOperandSide::Right);
            let Expression::Function(nested) = &expr.children[nested_idx] else {
                continue;
            };
            if nested.children.len() != 2
                || nested.function.stability != FunctionStability::Consistent
                || nested.function.side_effects != FunctionSideEffects::NoSideEffects
                || self
                    .shared_slots_by_identity
                    .contains(self.fingerprints.identity(&expr.children[nested_idx]))
            {
                continue;
            }
            for constant_side in [DecimalOperandSide::Left, DecimalOperandSide::Right] {
                let constant_idx = usize::from(constant_side == DecimalOperandSide::Right);
                let Expression::Constant(constant) = &nested.children[constant_idx] else {
                    continue;
                };
                let outer_variable_idx = usize::from(nested_side == DecimalOperandSide::Left);
                let inner_variable_idx = usize::from(constant_side == DecimalOperandSide::Left);
                let Some(function) = try_decimal_factor_fusion(
                    &expr.function,
                    &nested.function,
                    &constant.value,
                    &expr.children[outer_variable_idx].return_type(),
                    &nested.children[inner_variable_idx].return_type(),
                    &nested.children[constant_idx].return_type(),
                    nested_side,
                    constant_side,
                ) else {
                    continue;
                };
                return Some((
                    function,
                    [
                        &expr.children[outer_variable_idx],
                        &nested.children[inner_variable_idx],
                    ],
                ));
            }
        }
        None
    }
}

struct SharedExpressionCandidates<'a> {
    slots_by_identity: ExpressionIdentityRefMap<'a, usize>,
    slots: Vec<ExpressionScratchSlot>,
}

impl<'a> SharedExpressionCandidates<'a> {
    fn from_expressions(
        exprs: &[&'a Expression],
        fingerprints: &ExpressionFingerprintCatalog,
    ) -> Self {
        let mut raw_counts = ExpressionIdentityRefMap::<(usize, LogicalType)>::default();
        for expr in exprs {
            count_cse_candidates(expr, fingerprints, &mut raw_counts);
        }
        let mut raw_candidates = ExpressionIdentityRefSet::default();
        for (identity, (count, _)) in raw_counts.into_entries() {
            if count >= 2 {
                raw_candidates.insert(identity);
            }
        }

        // Count repeated work in the graph that will actually be evaluated.
        // Once a candidate subtree has been expanded, later occurrences are a
        // reference to that result; recursively recounting its descendants
        // would create redundant nested shared slots and block local kernels.
        let mut expanded = ExpressionIdentityRefSet::default();
        let mut counts = ExpressionIdentityRefMap::<(usize, LogicalType)>::default();
        for expr in exprs {
            count_effective_cse_candidates(
                expr,
                fingerprints,
                &raw_candidates,
                &mut expanded,
                &mut counts,
            );
        }

        let mut slots_by_identity = ExpressionIdentityRefMap::default();
        let mut slots = Vec::new();
        let mut counted = counts.into_entries().collect::<Vec<_>>();
        counted.sort_unstable_by_key(|(identity, _)| identity.fingerprint);
        for (identity, (count, return_type)) in counted {
            if count < 2 {
                continue;
            }
            let slot = slots.len();
            slots_by_identity.insert(identity, slot);
            slots.push(ExpressionScratchSlot { return_type });
        }

        Self {
            slots_by_identity,
            slots,
        }
    }
}

fn count_effective_cse_candidates<'a>(
    expr: &'a Expression,
    fingerprints: &ExpressionFingerprintCatalog,
    raw_candidates: &ExpressionIdentityRefSet<'a>,
    expanded: &mut ExpressionIdentityRefSet<'a>,
    counts: &mut ExpressionIdentityRefMap<'a, (usize, LogicalType)>,
) {
    ExpressionIterator::visit(expr, &mut |expr| {
        let identity = fingerprints.identity(expr);
        if !raw_candidates.contains(identity) {
            return ExpressionVisitDecision::Descend;
        }
        counts
            .get_or_insert_with(identity, || (0, expr.return_type()))
            .0 += 1;
        if expanded.insert(identity) {
            ExpressionVisitDecision::Descend
        } else {
            ExpressionVisitDecision::SkipChildren
        }
    });
}

fn count_cse_candidates<'a>(
    expr: &'a Expression,
    fingerprints: &ExpressionFingerprintCatalog,
    counts: &mut ExpressionIdentityRefMap<'a, (usize, LogicalType)>,
) {
    ExpressionIterator::visit(expr, &mut |expr| {
        if expression_cse_safe(expr) && expression_shareable(expr) {
            counts
                .get_or_insert_with(fingerprints.identity(expr), || (0, expr.return_type()))
                .0 += 1;
        }
        ExpressionVisitDecision::Descend
    });
}

fn expression_cse_safe(expr: &Expression) -> bool {
    match expr {
        Expression::Function(expr) => {
            expr.function.stability == FunctionStability::Consistent
                && expr.function.side_effects == FunctionSideEffects::NoSideEffects
                && expr.children.iter().all(expression_cse_safe)
        }
        Expression::Cast(expr) => expression_cse_safe(&expr.child),
        Expression::Comparison(expr) => {
            expression_cse_safe(&expr.left) && expression_cse_safe(&expr.right)
        }
        Expression::Conjunction(expr) => expr.children.iter().all(expression_cse_safe),
        Expression::Case(expr) => {
            expression_cse_safe(&expr.check)
                && expression_cse_safe(&expr.result_if_true)
                && expression_cse_safe(&expr.result_if_false)
        }
        Expression::Operator(expr) => expr.children.iter().all(expression_cse_safe),
        Expression::Constant(_) | Expression::ColumnRef(_) | Expression::Reference(_) => true,
        Expression::Parameter(_)
        | Expression::Aggregate(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => false,
    }
}

fn expression_shareable(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Function(_)
            | Expression::Cast(_)
            | Expression::Comparison(_)
            | Expression::Conjunction(_)
            | Expression::Case(_)
            | Expression::Operator(_)
    )
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;
    use paro_function::scalar::operators::arithmetic::register_arithmetic_functions;
    use paro_function::scalar::{
        ExpressionState, FunctionData, ScalarBindInput, ScalarFunction, ScalarFunctionSet,
    };
    use paro_planner::expression::{ConstantExpression, FunctionExpression, ReferenceExpression};

    fn bind_decimal(name: &str, arguments: &[LogicalType]) -> BoundScalarFunction {
        let mut set = ScalarFunctionSet::new(name.to_string());
        register_arithmetic_functions(&mut set);
        let (function, target_types) = set.bind(arguments).unwrap();
        function
            .bind(&ScalarBindInput::new(
                target_types,
                vec![None; arguments.len()],
            ))
            .unwrap()
    }

    fn reference(index: usize, ty: LogicalType) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, ty))
    }

    fn integer_one() -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Integer(1),
            return_type: LogicalType::Integer,
        })
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CollidingBindData(u8);

    impl FunctionData for CollidingBindData {
        fn clone_box(&self) -> Box<dyn FunctionData> {
            Box::new(self.clone())
        }

        fn equals(&self, other: &dyn FunctionData) -> bool {
            other.as_any().downcast_ref::<Self>() == Some(self)
        }

        fn fingerprint(&self) -> u64 {
            7
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn collision_dispatch(
        _input: &paro_common::chunk::Chunk,
        _state: &dyn ExpressionState,
        _result: &mut paro_common::vector::Vector,
    ) -> paro_common::error::Result<()> {
        Ok(())
    }

    fn colliding_expression(id: u8) -> Expression {
        let function = BoundScalarFunction::from(ScalarFunction::new(
            "collision_test".to_string(),
            vec![LogicalType::Integer],
            LogicalType::Integer,
            collision_dispatch,
        ))
        .with_bind_data(CollidingBindData(id));
        Expression::Function(FunctionExpression::new(
            function,
            vec![reference(0, LogicalType::Integer)],
            LogicalType::Integer,
        ))
    }

    #[test]
    fn expression_fingerprints_are_only_collision_buckets() {
        let first = colliding_expression(1);
        let second = colliding_expression(2);
        assert_eq!(
            expression_fingerprint(&first),
            expression_fingerprint(&second)
        );

        let program = PhysicalExpressionProgram::compile(
            &[first.clone(), second.clone()],
            ExpressionProgramVersion::anonymous(),
        );
        assert_eq!(program.unique_root_count(), 2);

        let expressions = [&first, &first, &second, &second];
        let fingerprints =
            ExpressionFingerprintCatalog::from_expressions(expressions.iter().copied());
        let candidates = SharedExpressionCandidates::from_expressions(&expressions, &fingerprints);
        assert_eq!(candidates.slots.len(), 2);

        let mut cache = ExpressionProgramCache::default();
        cache.get_or_compile(
            std::slice::from_ref(&first),
            ExpressionProgramVersion::anonymous(),
        );
        cache.get_or_compile(
            std::slice::from_ref(&second),
            ExpressionProgramVersion::anonymous(),
        );
        assert_eq!(cache.misses(), 2);
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn decimal_factor_fusion_preserves_shared_expression_boundaries() {
        let price_type = LogicalType::Decimal {
            precision: 15,
            scale: 2,
        };
        let factor_type = LogicalType::Decimal {
            precision: 4,
            scale: 2,
        };
        let discount = bind_decimal("-", &[LogicalType::Integer, factor_type.clone()]);
        let discount_expr = Expression::Function(FunctionExpression::new(
            discount.clone(),
            vec![integer_one(), reference(1, factor_type.clone())],
            discount.return_type.clone(),
        ));
        let discounted_price =
            bind_decimal("*", &[price_type.clone(), discount.return_type.clone()]);
        let discounted_price_expr = Expression::Function(FunctionExpression::new(
            discounted_price.clone(),
            vec![reference(0, price_type.clone()), discount_expr],
            discounted_price.return_type.clone(),
        ));

        // Bind an equivalent producer independently. Semantic bind-data
        // fingerprints, rather than Arc identity, must still expose the common
        // subexpression used by the output root and the charge expression.
        let discount_for_charge = bind_decimal("-", &[LogicalType::Integer, factor_type.clone()]);
        let discount_for_charge_expr = Expression::Function(FunctionExpression::new(
            discount_for_charge.clone(),
            vec![integer_one(), reference(1, factor_type.clone())],
            discount_for_charge.return_type.clone(),
        ));
        let discounted_price_for_charge = bind_decimal(
            "*",
            &[price_type.clone(), discount_for_charge.return_type.clone()],
        );
        let discounted_price_for_charge_expr = Expression::Function(FunctionExpression::new(
            discounted_price_for_charge.clone(),
            vec![reference(0, price_type), discount_for_charge_expr],
            discounted_price_for_charge.return_type.clone(),
        ));

        let tax = bind_decimal("+", &[factor_type.clone(), LogicalType::Integer]);
        let tax_expr = Expression::Function(FunctionExpression::new(
            tax.clone(),
            vec![reference(2, factor_type), integer_one()],
            tax.return_type.clone(),
        ));
        let charge = bind_decimal(
            "*",
            &[
                discounted_price.return_type.clone(),
                tax.return_type.clone(),
            ],
        );
        let charge_expr = Expression::Function(FunctionExpression::new(
            charge.clone(),
            vec![discounted_price_for_charge_expr, tax_expr],
            charge.return_type.clone(),
        ));

        let program = PhysicalExpressionProgram::compile(
            &[discounted_price_expr, charge_expr],
            ExpressionProgramVersion::anonymous(),
        );
        assert_eq!(program.shared_expression_count(), 1);
        let PhysicalExpression::Function(shared) = program.shared_node(0) else {
            panic!("shared discounted price should compile as a fused function")
        };
        assert_eq!(shared.function.name, "decimal_factor_fusion");

        let PhysicalExpression::Function(charge) = program.root(1) else {
            panic!("charge should compile as a fused function")
        };
        assert_eq!(charge.function.name, "decimal_factor_fusion");
        assert!(matches!(charge.children[0], PhysicalExpression::Shared(_)));
        let [chain] = program.decimal_factor_chains() else {
            panic!("expected one decimal factor chain")
        };
        assert_eq!(chain.producer_output, 0);
        assert_eq!(chain.consumer_output, 1);
        assert_eq!(chain.shared_slot, 0);
        assert_eq!(chain.consumer_shared_side, DecimalOperandSide::Left);
    }

    #[test]
    fn decimal_factor_product_expression_compiles_to_one_four_input_kernel() {
        let money = LogicalType::Decimal {
            precision: 15,
            scale: 2,
        };
        let rate = LogicalType::Decimal {
            precision: 4,
            scale: 2,
        };
        let discount = bind_decimal("-", &[LogicalType::Integer, rate.clone()]);
        let discount_expr = Expression::Function(FunctionExpression::new(
            discount.clone(),
            vec![integer_one(), reference(1, rate)],
            discount.return_type.clone(),
        ));
        let revenue = bind_decimal("*", &[money.clone(), discount.return_type.clone()]);
        let revenue_expr = Expression::Function(FunctionExpression::new(
            revenue.clone(),
            vec![reference(0, money.clone()), discount_expr],
            revenue.return_type.clone(),
        ));
        let cost = bind_decimal("*", &[money.clone(), money.clone()]);
        let cost_expr = Expression::Function(FunctionExpression::new(
            cost.clone(),
            vec![reference(2, money.clone()), reference(3, money)],
            cost.return_type.clone(),
        ));
        let profit = bind_decimal(
            "-",
            &[revenue.return_type.clone(), cost.return_type.clone()],
        );
        let profit_expr = Expression::Function(FunctionExpression::new(
            profit.clone(),
            vec![revenue_expr, cost_expr],
            profit.return_type.clone(),
        ));

        let program = PhysicalExpressionProgram::compile(
            &[profit_expr],
            ExpressionProgramVersion::anonymous(),
        );
        let PhysicalExpression::Function(root) = program.root(0) else {
            panic!("profit expression should compile to a function kernel")
        };
        assert_eq!(root.function.name, "decimal_factor_product_fusion");
        assert_eq!(root.children.len(), 4);
    }
}
