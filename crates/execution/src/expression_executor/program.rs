// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical expression program image and cache.
//!
//! EXPR-PROGRAM v1 intentionally uses a Velox-style vectorized expression tree with
//! typed kernel dispatch. It does not introduce bytecode or JIT; those can be
//! separate backends behind the same program/cache versioning later.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use paro_common::runtime_value::Value;
use paro_common::typed_parameters::ParameterSlot;
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_function::scalar::cast::{BoundCastInfo, CastDispatch};
use paro_function::scalar::{
    BoundScalarFunction, DictionaryStrategy, FunctionErrorMode, FunctionNullHandling,
    FunctionSideEffects, FunctionStability, ScalarDispatch,
};
use paro_planner::expression::{ComparisonType, ConjunctionType, Expression, OperatorType};

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
pub struct ExpressionProgramCacheKey {
    root_fingerprints: Box<[u64]>,
    backend: ExpressionBackend,
    physical_semantics_version: u32,
    visible_generation: u64,
    settings_fingerprint: u64,
}

impl ExpressionProgramCacheKey {
    pub fn new(exprs: &[Expression], version: &ExpressionProgramVersion) -> Self {
        Self::from_fingerprints(expression_list_fingerprints(exprs), version)
    }

    fn from_fingerprints(root_fingerprints: Vec<u64>, version: &ExpressionProgramVersion) -> Self {
        Self {
            root_fingerprints: root_fingerprints.into_boxed_slice(),
            backend: version.backend,
            physical_semantics_version: version.physical_semantics_version,
            visible_generation: version.visible_generation,
            settings_fingerprint: version.settings_fingerprint,
        }
    }

    pub fn root_fingerprints(&self) -> &[u64] {
        &self.root_fingerprints
    }
}

const DEFAULT_PROGRAM_CACHE_LIMIT: usize = 4096;

#[derive(Debug)]
pub struct ExpressionProgramCache {
    programs: HashMap<ExpressionProgramCacheKey, CachedProgramEntry>,
    lru: VecDeque<(ExpressionProgramCacheKey, u64)>,
    max_entries: usize,
    access_epoch: u64,
    hits: u64,
    misses: u64,
}

#[derive(Debug)]
struct CachedProgramEntry {
    program: Arc<PhysicalExpressionProgram>,
    epoch: u64,
}

impl Default for ExpressionProgramCache {
    fn default() -> Self {
        Self::with_capacity_limit(DEFAULT_PROGRAM_CACHE_LIMIT)
    }
}

impl ExpressionProgramCache {
    pub fn with_capacity_limit(max_entries: usize) -> Self {
        Self {
            programs: HashMap::new(),
            lru: VecDeque::new(),
            max_entries: max_entries.max(1),
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
        let key = ExpressionProgramCacheKey::new(exprs, &version);
        self.get_or_compile_with(key, |root_fingerprints| {
            PhysicalExpressionProgram::compile_with_fingerprints(exprs, version, root_fingerprints)
        })
    }

    pub(crate) fn get_or_compile_refs(
        &mut self,
        exprs: &[&Expression],
        version: ExpressionProgramVersion,
    ) -> Arc<PhysicalExpressionProgram> {
        let key = ExpressionProgramCacheKey::from_fingerprints(
            exprs
                .iter()
                .map(|expr| expression_fingerprint(expr))
                .collect(),
            &version,
        );
        self.get_or_compile_with(key, |root_fingerprints| {
            PhysicalExpressionProgram::compile_refs_with_fingerprints(
                exprs,
                version,
                root_fingerprints,
            )
        })
    }

    fn get_or_compile_with(
        &mut self,
        key: ExpressionProgramCacheKey,
        compile: impl FnOnce(Vec<u64>) -> PhysicalExpressionProgram,
    ) -> Arc<PhysicalExpressionProgram> {
        let epoch = self.next_epoch();
        if let Some(entry) = self.programs.get_mut(&key) {
            self.hits += 1;
            entry.epoch = epoch;
            let program = Arc::clone(&entry.program);
            self.lru.push_back((key, epoch));
            self.compact_stale_lru_if_needed();
            return program;
        }
        self.misses += 1;
        let program = Arc::new(compile(key.root_fingerprints.to_vec()));
        self.programs.insert(
            key.clone(),
            CachedProgramEntry {
                program: Arc::clone(&program),
                epoch,
            },
        );
        self.lru.push_back((key, epoch));
        self.evict_over_limit();
        self.compact_stale_lru_if_needed();
        program
    }

    fn next_epoch(&mut self) -> u64 {
        self.access_epoch = self.access_epoch.wrapping_add(1).max(1);
        self.access_epoch
    }

    fn evict_over_limit(&mut self) {
        while self.programs.len() > self.max_entries {
            let Some((victim, epoch)) = self.lru.pop_front() else {
                break;
            };
            if self
                .programs
                .get(&victim)
                .is_some_and(|entry| entry.epoch == epoch)
            {
                self.programs.remove(&victim);
            }
        }
    }

    fn compact_stale_lru_if_needed(&mut self) {
        if self.lru.len() <= self.max_entries.saturating_mul(4).max(16) {
            return;
        }
        let mut fresh = VecDeque::with_capacity(self.programs.len());
        for (key, entry) in &self.programs {
            fresh.push_back((key.clone(), entry.epoch));
        }
        self.lru = fresh;
    }

    #[cfg(test)]
    pub fn contains_program(
        &self,
        exprs: &[Expression],
        version: &ExpressionProgramVersion,
    ) -> bool {
        let key = ExpressionProgramCacheKey::new(exprs, version);
        self.programs.contains_key(&key)
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
        self.programs.len()
    }
}

pub fn expression_list_fingerprints(exprs: &[Expression]) -> Vec<u64> {
    exprs.iter().map(expression_fingerprint).collect()
}

pub fn expression_fingerprint(expr: &Expression) -> u64 {
    let mut hasher = StableExpressionHasher::new();
    hasher.hash_expression(expr);
    hasher.finish()
}

struct StableExpressionHasher {
    state: u64,
}

impl StableExpressionHasher {
    fn new() -> Self {
        Self {
            state: 0xcbf29ce484222325,
        }
    }

    fn finish(&self) -> u64 {
        self.state
    }

    fn tag(&mut self, value: u8) {
        self.write_u8(value);
    }

    fn hash_value<T: Hash>(&mut self, value: &T) {
        value.hash(self);
    }

    fn hash_str_value(&mut self, value: &str) {
        self.write_usize(value.len());
        self.write(value.as_bytes());
    }

    fn hash_exprs(&mut self, exprs: &[Expression]) {
        self.write_usize(exprs.len());
        for expr in exprs {
            self.hash_expression(expr);
        }
    }

    fn hash_function(&mut self, function: &BoundScalarFunction) {
        self.hash_str_value(&function.name);
        self.hash_value(&function.arguments);
        self.hash_value(&function.return_type);
        self.tag(function_stability_tag(function.stability));
        self.tag(function_null_handling_tag(function.null_handling));
        self.tag(function_side_effects_tag(function.side_effects));
        self.tag(function_error_mode_tag(function.error_mode));
        self.hash_dictionary_strategy(function.dictionary_strategy);
        self.hash_scalar_dispatch(function.dispatch);
        match function.init_local_state {
            Some(init) => {
                self.tag(1);
                self.write_usize(init as usize);
            }
            None => self.tag(0),
        }
        match &function.bind_data {
            Some(data) => {
                self.tag(1);
                self.write_usize(Arc::as_ptr(data) as *const () as usize);
            }
            None => self.tag(0),
        }
    }

    fn hash_scalar_dispatch(&mut self, dispatch: ScalarDispatch) {
        match dispatch {
            ScalarDispatch::Direct(function) => {
                self.tag(0);
                self.write_usize(function as usize);
            }
            ScalarDispatch::Variadic(function) => {
                self.tag(1);
                self.write_usize(function as usize);
            }
        }
    }

    fn hash_dictionary_strategy(&mut self, strategy: DictionaryStrategy) {
        match strategy {
            DictionaryStrategy::Materialize => self.tag(0),
            DictionaryStrategy::StorageDictionaryCache { input_idx } => {
                self.tag(1);
                self.write_usize(input_idx);
            }
        }
    }

    fn hash_cast(&mut self, cast: &BoundCastInfo) {
        match cast.dispatch {
            CastDispatch::Fixed(function) => {
                self.tag(0);
                self.write_usize(function as usize);
            }
            CastDispatch::Varlen(function) => {
                self.tag(1);
                self.write_usize(function as usize);
            }
            CastDispatch::Array(function) => {
                self.tag(2);
                self.write_usize(function as usize);
            }
            CastDispatch::Struct(function) => {
                self.tag(3);
                self.write_usize(function as usize);
            }
        }
        match &cast.cast_data {
            Some(data) => {
                self.tag(1);
                self.write_usize(Arc::as_ptr(data) as *const () as usize);
            }
            None => self.tag(0),
        }
    }

    fn hash_expression(&mut self, expr: &Expression) {
        match expr {
            Expression::Constant(expr) => {
                self.tag(0);
                self.hash_value(&expr.return_type);
                self.hash_value(&expr.value);
            }
            Expression::ColumnRef(expr) => {
                self.tag(1);
                self.hash_value(&expr.binding);
                self.hash_value(&expr.return_type);
                self.write_usize(expr.depth);
            }
            Expression::Function(expr) => {
                self.tag(2);
                self.hash_function(&expr.function);
                self.hash_value(&expr.return_type);
                self.hash_exprs(&expr.children);
            }
            Expression::Cast(expr) => {
                self.tag(3);
                self.hash_expression(&expr.child);
                self.hash_value(&expr.target_type);
                self.write_u8(u8::from(expr.try_cast));
                self.hash_cast(&expr.cast_info);
            }
            Expression::Conjunction(expr) => {
                self.tag(4);
                self.tag(conjunction_tag(expr.conjunction_type));
                self.hash_exprs(&expr.children);
            }
            Expression::Case(expr) => {
                self.tag(5);
                self.hash_expression(&expr.check);
                self.hash_expression(&expr.result_if_true);
                self.hash_expression(&expr.result_if_false);
                self.hash_value(&expr.return_type);
            }
            Expression::Comparison(expr) => {
                self.tag(6);
                self.tag(comparison_tag(expr.comparison_type));
                self.hash_expression(&expr.left);
                self.hash_expression(&expr.right);
            }
            Expression::Operator(expr) => {
                self.tag(7);
                self.tag(operator_tag(expr.operator_type));
                self.hash_value(&expr.return_type);
                self.hash_exprs(&expr.children);
            }
            Expression::Parameter(expr) => {
                self.tag(8);
                self.write_usize(expr.slot.index.index());
                self.hash_value(&expr.slot.ty);
            }
            Expression::Reference(expr) => {
                self.tag(9);
                self.write_usize(expr.index);
                self.hash_value(&expr.return_type);
            }
            Expression::Aggregate(_) => {
                self.tag(10);
            }
            Expression::Subquery(_) => {
                self.tag(11);
            }
            Expression::Window(_) => {
                self.tag(12);
            }
        }
    }
}

impl Hasher for StableExpressionHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(0x100000001b3);
        }
    }
}

fn function_stability_tag(value: FunctionStability) -> u8 {
    match value {
        FunctionStability::Consistent => 0,
        FunctionStability::ConsistentWithinQuery => 1,
        FunctionStability::Volatile => 2,
    }
}

fn function_null_handling_tag(value: FunctionNullHandling) -> u8 {
    match value {
        FunctionNullHandling::DefaultNullHandling => 0,
        FunctionNullHandling::SpecialHandling => 1,
    }
}

fn function_side_effects_tag(value: FunctionSideEffects) -> u8 {
    match value {
        FunctionSideEffects::NoSideEffects => 0,
        FunctionSideEffects::HasSideEffects => 1,
    }
}

fn function_error_mode_tag(value: FunctionErrorMode) -> u8 {
    match value {
        FunctionErrorMode::CanError => 0,
        FunctionErrorMode::Infallible => 1,
    }
}

fn conjunction_tag(value: ConjunctionType) -> u8 {
    match value {
        ConjunctionType::And => 0,
        ConjunctionType::Or => 1,
    }
}

fn comparison_tag(value: ComparisonType) -> u8 {
    match value {
        ComparisonType::Equal => 0,
        ComparisonType::NotEqual => 1,
        ComparisonType::LessThan => 2,
        ComparisonType::LessThanOrEqual => 3,
        ComparisonType::GreaterThan => 4,
        ComparisonType::GreaterThanOrEqual => 5,
        ComparisonType::DistinctFrom => 6,
        ComparisonType::NotDistinctFrom => 7,
    }
}

fn operator_tag(value: OperatorType) -> u8 {
    match value {
        OperatorType::Not => 0,
        OperatorType::IsNull => 1,
        OperatorType::IsNotNull => 2,
        OperatorType::Like => 3,
        OperatorType::ILike => 4,
        OperatorType::In => 5,
        OperatorType::NotIn => 6,
        OperatorType::Coalesce => 7,
        OperatorType::ArrayConstructor => 8,
        OperatorType::ArrayExtract => 9,
        OperatorType::StructConstructor => 10,
        OperatorType::ErrorIfMultipleRows => 11,
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalExpressionProgram {
    roots: Vec<PhysicalExpression>,
    shared_nodes: Vec<PhysicalExpression>,
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
        let shared_candidates = SharedExpressionCandidates::from_expressions(exprs.clone());
        let mut compiler = ProgramCompiler::new(shared_candidates);
        let mut root_to_unique = Vec::with_capacity(root_count);
        let mut root_first_output = Vec::with_capacity(root_count);
        let mut unique_by_key = HashMap::new();
        let mut roots = Vec::new();

        debug_assert_eq!(root_count, root_fingerprints.len());
        for (expr, root_fingerprint) in exprs.zip(root_fingerprints.iter().copied()) {
            let compiled = compiler.compile_expression(expr);
            if compiled.cse_safe {
                let unique = *unique_by_key.entry(root_fingerprint).or_insert_with(|| {
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

        Self {
            roots,
            shared_nodes: compiler
                .shared_nodes
                .into_iter()
                .map(|node| node.expect("shared expression candidate was not compiled"))
                .collect(),
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
    pub fingerprint: u64,
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

struct ProgramCompiler {
    shared_slots_by_fingerprint: HashMap<u64, usize>,
    shared_nodes: Vec<Option<PhysicalExpression>>,
    scratch_slots: Vec<ExpressionScratchSlot>,
    compiling_shared: HashSet<u64>,
}

struct CompiledExpr {
    expr: PhysicalExpression,
    cse_safe: bool,
}

impl ProgramCompiler {
    fn new(shared_candidates: SharedExpressionCandidates) -> Self {
        Self {
            shared_slots_by_fingerprint: shared_candidates.slots_by_fingerprint,
            shared_nodes: vec![None; shared_candidates.slots.len()],
            scratch_slots: shared_candidates.slots,
            compiling_shared: HashSet::new(),
        }
    }

    fn compile_expression(&mut self, expr: &Expression) -> CompiledExpr {
        let fingerprint = expression_fingerprint(expr);
        if let Some(&slot) = self.shared_slots_by_fingerprint.get(&fingerprint) {
            if !self.compiling_shared.contains(&fingerprint) {
                if self.shared_nodes[slot].is_none() {
                    self.compiling_shared.insert(fingerprint);
                    let compiled = self.compile_expression_inner(expr);
                    self.compiling_shared.remove(&fingerprint);
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

    fn compile_expression_inner(&mut self, expr: &Expression) -> CompiledExpr {
        match expr {
            Expression::Function(expr) => {
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
}

struct SharedExpressionCandidates {
    slots_by_fingerprint: HashMap<u64, usize>,
    slots: Vec<ExpressionScratchSlot>,
}

impl SharedExpressionCandidates {
    fn from_expressions<'a>(exprs: impl Iterator<Item = &'a Expression>) -> Self {
        let mut counts = HashMap::<u64, (usize, LogicalType)>::new();
        for expr in exprs {
            count_cse_candidates(expr, &mut counts);
        }

        let mut slots_by_fingerprint = HashMap::new();
        let mut slots = Vec::new();
        let mut counted = counts.into_iter().collect::<Vec<_>>();
        counted.sort_unstable_by_key(|(fingerprint, _)| *fingerprint);
        for (fingerprint, (count, return_type)) in counted {
            if count < 2 {
                continue;
            }
            let slot = slots.len();
            slots_by_fingerprint.insert(fingerprint, slot);
            slots.push(ExpressionScratchSlot {
                return_type,
                fingerprint,
            });
        }

        Self {
            slots_by_fingerprint,
            slots,
        }
    }
}

fn count_cse_candidates(expr: &Expression, counts: &mut HashMap<u64, (usize, LogicalType)>) {
    if expression_cse_safe(expr) && expression_shareable(expr) {
        let fingerprint = expression_fingerprint(expr);
        let entry = counts
            .entry(fingerprint)
            .or_insert_with(|| (0, expr.return_type()));
        entry.0 += 1;
    }

    match expr {
        Expression::Function(expr) => {
            for child in &expr.children {
                count_cse_candidates(child, counts);
            }
        }
        Expression::Cast(expr) => count_cse_candidates(&expr.child, counts),
        Expression::Comparison(expr) => {
            count_cse_candidates(&expr.left, counts);
            count_cse_candidates(&expr.right, counts);
        }
        Expression::Conjunction(expr) => {
            for child in &expr.children {
                count_cse_candidates(child, counts);
            }
        }
        Expression::Case(expr) => {
            count_cse_candidates(&expr.check, counts);
            count_cse_candidates(&expr.result_if_true, counts);
            count_cse_candidates(&expr.result_if_false, counts);
        }
        Expression::Operator(expr) => {
            for child in &expr.children {
                count_cse_candidates(child, counts);
            }
        }
        Expression::Constant(_)
        | Expression::ColumnRef(_)
        | Expression::Parameter(_)
        | Expression::Reference(_)
        | Expression::Aggregate(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => {}
    }
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
