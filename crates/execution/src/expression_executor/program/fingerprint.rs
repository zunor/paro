// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bottom-up semantic fingerprints for bound scalar expressions.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use paro_function::scalar::cast::{BoundCastInfo, CastContextDependency, CastDispatch};
use paro_function::scalar::{
    BoundScalarFunction, DictionaryStrategy, FunctionErrorMode, FunctionNullHandling,
    FunctionSideEffects, FunctionStability, ScalarDispatch,
};
use paro_planner::expression::{
    ComparisonType, ConjunctionType, Expression, ExpressionIterator, OperatorType,
};

use super::identity::ExpressionIdentityRef;

/// Fingerprints every physical node once and reuses child digests when hashing
/// its parent. The catalog is scoped to one immutable bound-expression forest,
/// so pointer identity is only an internal lookup accelerator.
pub(super) struct ExpressionFingerprintCatalog {
    by_address: HashMap<usize, ExpressionNodeDigest>,
}

#[derive(Clone, Copy)]
struct ExpressionNodeDigest {
    fingerprint: u64,
    subtree_nodes: usize,
}

impl ExpressionFingerprintCatalog {
    pub(super) fn from_expressions<'a>(
        expressions: impl IntoIterator<Item = &'a Expression>,
    ) -> Self {
        let mut catalog = Self {
            by_address: HashMap::new(),
        };
        for expression in expressions {
            catalog.insert(expression);
        }
        catalog
    }

    pub(super) fn fingerprint(&self, expression: &Expression) -> u64 {
        self.digest(expression).fingerprint
    }

    pub(super) fn retained_nodes<'a>(
        &self,
        expressions: impl IntoIterator<Item = &'a Expression>,
    ) -> usize {
        expressions
            .into_iter()
            .map(|expression| self.digest(expression).subtree_nodes)
            .sum()
    }

    pub(super) fn identity<'a>(&self, expression: &'a Expression) -> ExpressionIdentityRef<'a> {
        ExpressionIdentityRef::new(expression, self.fingerprint(expression))
    }

    fn digest(&self, expression: &Expression) -> ExpressionNodeDigest {
        self.by_address[&expression_address(expression)]
    }

    fn insert(&mut self, expression: &Expression) -> ExpressionNodeDigest {
        let address = expression_address(expression);
        if let Some(digest) = self.by_address.get(&address) {
            return *digest;
        }

        let mut child_fingerprints = Vec::new();
        let mut subtree_nodes = 1usize;
        ExpressionIterator::enumerate_children(expression, |child| {
            let child = self.insert(child);
            child_fingerprints.push(child.fingerprint);
            subtree_nodes = subtree_nodes.saturating_add(child.subtree_nodes);
        });
        let mut hasher = StableExpressionHasher::new();
        hasher.hash_expression_node(expression, &child_fingerprints);
        let digest = ExpressionNodeDigest {
            fingerprint: hasher.finish(),
            subtree_nodes,
        };
        self.by_address.insert(address, digest);
        digest
    }
}

pub fn expression_list_fingerprints(expressions: &[Expression]) -> Vec<u64> {
    let catalog = ExpressionFingerprintCatalog::from_expressions(expressions);
    expressions
        .iter()
        .map(|expression| catalog.fingerprint(expression))
        .collect()
}

pub fn expression_fingerprint(expression: &Expression) -> u64 {
    ExpressionFingerprintCatalog::from_expressions([expression]).fingerprint(expression)
}

fn expression_address(expression: &Expression) -> usize {
    std::ptr::from_ref(expression) as usize
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

    fn hash_child_fingerprints(&mut self, fingerprints: &[u64]) {
        self.write_usize(fingerprints.len());
        for fingerprint in fingerprints {
            self.write_u64(*fingerprint);
        }
    }

    fn hash_function(&mut self, function: &BoundScalarFunction) {
        self.hash_str_value(&function.name);
        self.hash_value(&function.arguments);
        self.hash_value(&function.return_type);
        self.hash_value(&function.varargs);
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
                self.write_u64(data.fingerprint());
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
        self.tag(match cast.context_dependency() {
            CastContextDependency::Independent => 0,
            CastContextDependency::Runtime => 1,
        });
        match &cast.cast_data {
            Some(data) => {
                self.tag(1);
                self.write_usize(Arc::as_ptr(data) as *const () as usize);
            }
            None => self.tag(0),
        }
    }

    fn hash_expression_node(&mut self, expression: &Expression, child_fingerprints: &[u64]) {
        match expression {
            Expression::Constant(expression) => {
                self.tag(0);
                self.hash_value(&expression.return_type);
                self.hash_value(&expression.value);
            }
            Expression::ColumnRef(expression) => {
                self.tag(1);
                self.hash_value(&expression.binding);
                self.hash_value(&expression.return_type);
                self.write_usize(expression.depth);
            }
            Expression::Function(expression) => {
                self.tag(2);
                self.hash_function(&expression.function);
                self.hash_value(&expression.return_type);
            }
            Expression::Cast(expression) => {
                self.tag(3);
                self.hash_value(&expression.target_type);
                self.write_u8(u8::from(expression.try_cast));
                self.hash_cast(&expression.cast_info);
            }
            Expression::Conjunction(expression) => {
                self.tag(4);
                self.tag(conjunction_tag(expression.conjunction_type));
            }
            Expression::Case(expression) => {
                self.tag(5);
                self.hash_value(&expression.return_type);
            }
            Expression::Comparison(expression) => {
                self.tag(6);
                self.tag(comparison_tag(expression.comparison_type));
            }
            Expression::Operator(expression) => {
                self.tag(7);
                self.tag(operator_tag(expression.operator_type));
                self.hash_value(&expression.return_type);
            }
            Expression::Parameter(expression) => {
                self.tag(8);
                self.write_usize(expression.slot.index.index());
                self.hash_value(&expression.slot.ty);
            }
            Expression::Reference(expression) => {
                self.tag(9);
                self.write_usize(expression.index);
                self.hash_value(&expression.return_type);
            }
            Expression::Aggregate(_) => self.tag(10),
            Expression::Subquery(_) => self.tag(11),
            Expression::Window(_) => self.tag(12),
        }
        self.hash_child_fingerprints(child_fingerprints);
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
